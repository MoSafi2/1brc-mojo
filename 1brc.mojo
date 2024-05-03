from math import trunc
from algorithm.sort import sort
from string_dict import Dict as CompactDict
from algorithm import parallelize
from algorithm.functional import sync_parallelize
from os import SEEK_CUR
import os.fstat
import sys
from utils import Variant, InlineArray
from memory import memset

alias input_file = "measurements_100M_C.txt"
# alias input_file = "small_measurements.txt"
alias HCAP = 4096 * 10
alias cores = 32
alias MorN = Variant[Measurement, NoneType]

@value
struct Measurement(CollectionElement, Stringable):
    var name: String
    var min: Int16
    var max: Int16
    var sum: Int
    var count: Int

    fn __add__(inout self, other: Self) raises -> Self :

        if self.name != other.name:
            raise Error("Measurements are not the same")
        var new = Measurement(
            name = self.name,
            min = min(self.min, other.min),
            max = max(self.max, other.max),
            sum = self.sum + other.sum,
            count = self.count + other.count
        )
        return new

    fn __str__(self) -> String:
        return (
            String("name")
            + self.name
            + "\nMax:"
            + self.max
            + "\nmin"
            + self.min
            + "\nsum"
            + self.sum
            + "\ncount"
            + self.count
        )


@always_inline
fn raw_to_float(raw_value: StringRef) -> Int16:
    var p = raw_value.data

    var x: SIMD[DType.int8, 4]
    if raw_value[0] == "-":
        x = raw_value.data.load[width=4](1) - 48
    else:
        x = raw_value.data.load[width=4](0) - 48

    var mask = x >= 0 and x <= 9

    var val: Int16 = 0
    for i in range(0, 4, 1):
        if mask[i]:
            val = val * 10 + int(x[i])
    if raw_value[0] == "-":
        val = val * -1
    return val


fn format_float(value: Float32) -> String:
    return String(int(trunc(value))) + "." + int(abs(value * 10) % 10)


@always_inline
fn format_int(value: Int) -> String:
    var sign = ""
    if value < 0:
        sign = "-"
    return sign + String(abs(value) // 10) + "." + abs(value) % 10


@always_inline
fn swap(inout vector: List[String], a: Int, b: Int):
    var tmp = vector[a]
    vector[a] = vector[b]
    vector[b] = tmp


@always_inline
fn tagger[
    num_workers: Int
](chunk: StringRef, substr: StringRef = "\n") -> List[Int]:

    var indicies = List[Int]()
    indicies.append(0)
    var last_index = chunk.rfind(substr)
    var leap = int(last_index / num_workers)
    var offset = 0
    for i in range(num_workers):
        indicies.append(chunk.find(substr, offset + leap))
        offset += leap
    return indicies


@always_inline
fn process_line(line: StringRef, inout aggregator: CompactDict[Measurement]):
    var name_loc = line.find(";")
    var name = StringRef(line.data, name_loc + 1)
    var raw_value = StringRef(line.data + name_loc + 1, len(line) - len(name))
    var value = raw_to_float(raw_value)

    # Maybe can be streamlined?
    # var measurement = aggregator.get(
    #     name, default=Measurement(name, value, value, 0, 0)
    # )
    var measurement = Measurement(name, value, value, 0, 0)
    measurement.min = min(measurement.min, value)
    measurement.max = max(measurement.max, value)
    measurement.sum += int(value)
    measurement.count += 1
    # aggregator.put(name, measurement)


fn process_line2(line: StringRef, inout aggregator: UnsafePointer[MorN]):
    var hash: UInt64 = 0
    var pos: Int = -1
    for i in range(len(line)):
        if line[i] != ";":
            hash = hash * 31 + int(line[i]._as_ptr().load())
        else:
            pos = i
    hash = hash & (HCAP - 1)

    var name = StringRef(line._as_ptr(), pos+1)
    var raw_value = StringRef(line._as_ptr() + pos + 1, len(line) - len(name))
    var value = raw_to_float(raw_value)
    var measurement = Measurement(name, value, value, int(value), 1)

    while True:
        var x = aggregator[int(hash)]
        if x.isa[NoneType]():
            aggregator[int(hash)] = measurement
            break
        if aggregator[int(hash)].get[Measurement]()[].name == name:
            var m1 = aggregator[int(hash)].get[Measurement]()[]
            try:
                aggregator[int(hash)] =  m1 + measurement
                break
            except Error:
                print(Error, m1.name, measurement.name)
                break
        else:
            hash =(hash + 1) & (HCAP - 1)
            

    # if key == 0:
    #     measurement = Measurement(name, value, value, 0,0)
    # else:
    #     measurement =  aggregator.values[key - 1]
    

    # Maybe can be streamlined?
    #  measurement = aggregator.get(
    #     name, default=Measurement(name, value, value, 0, 0)
    # )
    # aggregator.put(name, measurement)



@always_inline
fn worker(chunk: StringRef, inout aggregator: UnsafePointer[MorN]):
    var p = chunk.data
    var head = 0
    var max = int(chunk.data.address + chunk.length)
    while True:
        var line_loc = chunk.find("\n", head)

        if line_loc == -1:
            break

        if line_loc > max:
            break

        var line = StringRef(p + head, line_loc - head)
        process_line2(line, aggregator)
        head = line_loc + 1

@always_inline
fn parallelizer[workers: Int](chunk: StringRef, inout aggr_list: List[UnsafePointer[MorN]]) -> Int:
    var indcies = tagger[workers](chunk)
    @parameter
    fn inner(index: Int):
        var str_ref = StringRef(chunk.data + indcies[index], indcies[index+1] - indcies[index])
        worker(str_ref, aggr_list[index])
    parallelize[inner](workers)
    
    return indcies[-1]
    #return 0

# Max read size in Mojo is 2GB
alias MAX_CHUNK = 2_000_000_000

fn main() raises:
    var consumed: UInt64 = 0
    var f = open(input_file, "r")

    var stat = fstat.stat(input_file)
    var size =  stat.st_size 

    var loops = size // MAX_CHUNK
    
    var x = UnsafePointer[MorN]().alloc(HCAP)

    var aggr_list = List[UnsafePointer[MorN]]()

    for i in range(cores):
        var x = UnsafePointer[MorN].alloc(HCAP)
        for i in range(HCAP):
            x[i] = None
        aggr_list.append(x)

    var buf = DTypePointer[DType.int8]().alloc(MAX_CHUNK)
    var pos: UInt64 = 0
    for i in range(loops):
        var chunk = f.read(buf, size = MAX_CHUNK)
        var ref = StringRef(buf, MAX_CHUNK)
        var aggregators = parallelizer[workers = cores](ref, aggr_list)
        pos = f.seek(aggregators - MAX_CHUNK, 1)
    buf.free()

    # # Handling last chunk
    var rem = int(size - pos)
    buf = DTypePointer[DType.int8]().alloc(int(rem))
    var chunk = f.read(buf, size = rem)
    var ref = StringRef(buf, rem)
    var aggregators = parallelizer[workers = cores](ref, aggr_list)

    # var master_dict = CompactDict[Measurement](capacity = 2000)
    # for i in range(len(aggr_list)):
    #     var dic = aggr_list[i]
    #     for j in range(dic.count):
    #         var meas = dic.values[j]
    #         var val = master_dict.get(meas.name, default = Measurement(meas.name, 0,0,0,0))
    #         meas = meas + val
    #         master_dict.put(meas.name, meas)

    # var names = List[String]()
    # for m in master_dict.values:
    #     names.append(m[].name)

    # var res: String = "{"
    # for name in names:
    #     var measurement = master_dict.get(name[], default=Measurement(name[], 0, 0, 0, 0))
    #     res += measurement.name + "=" + format_int(int(measurement.min)) + "/" + format_float((measurement.sum / measurement.count) / 10) + "/" + format_int(int(measurement.max)) + ", "
    # res += "}"
    # print(res)



