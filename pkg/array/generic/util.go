package array

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func mustVal[T any](val T, err error) T {
	if err != nil {
		panic(err)
	}
	return val
}
