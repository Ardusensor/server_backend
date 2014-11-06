package main

import (
	"fmt"
	"os"
	"os/exec"
)

func main() {
	// Check that imagemagick is installed
	b, err := exec.Command("imagemagick").CombinedOutput()
	if err != nil {
		fmt.Println("Check that you have imagemagick installed")
		fmt.Println(string(b))
		os.Exit(1)
	}
	os.Exit(0)
}
