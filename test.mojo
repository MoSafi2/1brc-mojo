from math.math import abs, min, max, trunc, round
from algorithm.sort import sort
from string_dict import Dict as CompactDict
from algorithm import parallelize
from algorithm.functional import sync_parallelize
from os import SEEK_CUR
import os.fstat
import sys

alias input_file = "measurements_100M.txt"
# alias input_file = "small_measurements.txt"

alias cores = 8

@value
struct Measurement(Stringable):
    var name: String
    var min: Int16
    var max: Int16
    var sum: Int
    var count: Int

    fn __add__(inout self, other: Self) raises -> Self:

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
fn tagger[num_workers: Int](handle: FileHandle, substr: StringRef = "\n") raises -> List[Int]:
    
    var stat = fstat.stat(input_file)
    var size =  stat.st_size 
    var indicies = List[Int]()
    indicies.append(0)

    var leap = int(size / num_workers)
    var offset = 0
    for i in range(num_workers - 1):
        var offset = handle.seek(leap - 100, 1)
        var s = handle.read(100)
        indicies.append(int(offset + s.find(substr)))

    indicies.append(size)
    return indicies

@always_inline
fn process_line(line: StringRef, inout aggregator: CompactDict[Measurement]):
    var name_loc = line.find(";")
    var name = StringRef(line.data, name_loc + 1)
    var raw_value = StringRef(line.data + name_loc + 1, len(line) - len(name))
    var value = raw_to_float(raw_value)

    # Maybe can be streamlined?
    var measurement = aggregator.get(
        name, default=Measurement(name, value, value, 0, 0)
    )
    measurement.min = min(measurement.min, value)
    measurement.max = max(measurement.max, value)
    measurement.sum += int(value)
    measurement.count += 1
    aggregator.put(name, measurement)


@always_inline
fn worker(input_file: String, offset: Int, amt: Int,  inout aggregator: CompactDict[Measurement]):
    try:
        var handle = open(input_file, "r")  
        var chunk = handle.read(amt)
        var max = len(chunk)
        var head = 0
        var p = chunk._buffer.data

        while True:
            var line_loc = chunk.find("\n", head)

            if line_loc == -1:
                break

            if line_loc > max:
                break

            var line = StringRef(p + head, line_loc - head)
            process_line(line, aggregator)
            head = line_loc + 1
        _ = chunk
    except Error:
        print(Error)
        return

@always_inline
fn parallelizer[workers: Int](input_path: String) raises -> List[CompactDict[Measurement]]:

    var handle = open(input_path, "r")
    var indcies = tagger[workers](handle)
    var aggr_list = List[CompactDict[Measurement]]()
    for i in range(workers):
        aggr_list.append(CompactDict[Measurement]())
    @parameter
    fn inner(index: Int):
        worker(input_path, indcies[index], indcies[index+1] - indcies[index], aggr_list[index])
    parallelize[inner](workers)
    _ = aggr_list
    return aggr_list

fn main() raises:
    var aggregators = parallelizer[cores](input_file)
    var master_dict = CompactDict[Measurement](capacity = 2000)
    
    # for i in range(len(aggregators)):
    #     var dic = aggregators[i]
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

