package app

import "testing"

func TestSafeFilename(t *testing.T) {
	for input, want := range map[string]string{"../report.pdf": "report.pdf", `..\..\evil.exe`: "evil.exe", "CON.txt": "_CON.txt", "LPT1": "_LPT1", "x:y?.txt": "x_y_.txt", "...": "download.bin", "资料.zip": "资料.zip"} {
		if got := SafeFilename(input); got != want {
			t.Fatalf("%q => %q want %q", input, got, want)
		}
	}
}
