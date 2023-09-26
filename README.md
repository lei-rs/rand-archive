# Random Access Archive
.raa files are essentially a dict header + consecutive bytes of the samples. It was made to faccilitate and accelerate deep learning training on large datasets. It's written in Rust and fast, but easily accesible programmatically in Python. Most importantly, it allows you to shuffle the data, without sacrificing too much on sequential reads, by shuffling blocks of contiguous data. It also allows for lazy sharding. 

# Comparison
The main advantage of this library, is how extensible it is. Other libraries like Webdataset, FFCV, Streaming Dataset, TF Record, are very batteries included, which is great for experimentation, but sacrifices on extensibility heavily since they also include data processing. Our philosiphy quite simple, you write string byte pairs, and you read string byte pairs. We only implement functionality that NEEDS to be implemented at the reader level for optimization, like shuffling and sharding. 

## Benchmarks:
!todo

# Usage
```
pip install rand-archive
```

## Writing:
```python
from rand_archive import Writer

with Writer("test.raa") as w:
  w.write("test", bytes("test"))
```
## Reading
```python
from rand_archive import Reader

for _ in Reader().open_file("dummy.raa").with_shuffling():
  pass
```
