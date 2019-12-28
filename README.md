# Protobuf 3 column storage (Dremel) in C++14

Note: This is still WIP.

This is an ongoing effort to implement Protobuf column storage in C++. The format and algorithm is illustrated in [Google's Dremel paper](https://research.google/pubs/pub36632/). [Parquet](https://blog.twitter.com/engineering/en_us/a/2013/dremel-made-simple-with-parquet.html) adapts the same idea.

Required libraries:
- Protobuf 3
- Google test
- Glog
