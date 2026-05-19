package helper

func ConvertBoolToByte(b bool) byte {
	if b == true {
		return 1
	}

	return 0
}

func ConvertByteToBool(b byte) bool {
	if b == 1 {
		return true
	}

	return false
}
