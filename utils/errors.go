package utils

import "errors"

func GetRootError(err error) error {
	for {
		rootError := errors.Unwrap(err)
		if rootError == nil {
			return err
		}
		err = rootError
	}
}