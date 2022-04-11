package util

type UInt32 map[uint32]struct{}

type String map[string]struct{}

func UInt32Set2Arr(set UInt32) []uint32 {
	arr := make([]uint32, 0, len(set))
	for item := range set {
		arr = append(arr, item)
	}
	return arr
}
