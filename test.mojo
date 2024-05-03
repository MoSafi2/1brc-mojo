from utils import InlineArray, Variant

@value
struct Measurement:
    var name: Int32
    var min: Int16
    var max: Int16
    var sum: Int
    var count: Int


alias morn = Variant[Measurement, NoneType]

fn main():
    var x = InlineArray[morn, 50](None)
    if x[50].isa[NoneType]():
        print("None")


