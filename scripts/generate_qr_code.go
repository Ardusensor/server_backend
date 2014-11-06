package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
)

func main() {
	// Check that imagemagick is installed
	b, err := exec.Command("convert").CombinedOutput()
	if err != nil {
		fmt.Println("Check that you have imagemagick installed")
		fmt.Println(string(b))
		os.Exit(1)
	}

	// check that qrencode is installed
	b, err = exec.Command("qrencode").CombinedOutput()
	if err != nil {
		fmt.Println("Check that you have qrencode installed")
		fmt.Println(string(b))
		os.Exit(1)
	}

	// create output folder
	os.Mkdir("qrcodes", 0777)

	series := "1A"
	start := 1
	end := 100

	for i := start; i <= end; i++ {
		code := fmt.Sprintf("%s%03d", series, i)
	
		filename := filepath.Join("qrcodes", fmt.Sprintf("%s.png", code))
		content := fmt.Sprintf("{sensor_id=%s}", code)

		// generate qr code file
       		b, err = exec.Command("qrencode",
			"-s5", "-lH", "-o", filename, content).CombinedOutput()
		if err != nil {
			fmt.Println(string(b))
			os.Exit(1)
		}

		// place text on it
	}
}
