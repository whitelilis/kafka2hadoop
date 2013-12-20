shuffle.jar : InputGenerator.java  OffsetRange.java  OffsetSplitMapper.java  Shuffle.java ShuffleReducer.java
	rm -rf cn
	javac -d . InputGenerator.java  OffsetRange.java  OffsetSplitMapper.java  Shuffle.java ShuffleReducer.java
	jar cvf shuffle.jar cn kafka scala com
