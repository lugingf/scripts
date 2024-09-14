package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	md "github.com/JohannesKaufmann/html-to-markdown"
)

func runConverter() {
	inputDir := "/Users/evgeny.lugin/Downloads/CIPO" // Replace with the path to your HTML files
	outputDir := "./md_files"                        // Replace with the path for the output Markdown files

	if err := os.MkdirAll(outputDir, os.ModePerm); err != nil {
		panic(err)
	}

	htmlFiles, err := filepath.Glob(filepath.Join(inputDir, "*.html"))
	if err != nil {
		panic(err)
	}

	for _, htmlFile := range htmlFiles {
		convertHTMLToMarkdown(htmlFile, inputDir, outputDir)
	}

	fmt.Println("Conversion complete for all files.")
}

func convertHTMLToMarkdown(htmlFile, inputDir, outputDir string) {
	baseName := strings.TrimSuffix(filepath.Base(htmlFile), filepath.Ext(htmlFile))
	outputFilePath := filepath.Join(outputDir, baseName+".md")

	htmlContent, err := ioutil.ReadFile(htmlFile)
	if err != nil {
		fmt.Printf("Error reading file %s: %v\n", htmlFile, err)
		return
	}

	converter := md.NewConverter("", true, nil)
	markdownContent, err := converter.ConvertString(string(htmlContent))
	if err != nil {
		fmt.Printf("Error converting file %s to Markdown: %v\n", htmlFile, err)
		return
	}

	err = ioutil.WriteFile(outputFilePath, []byte(markdownContent), 0644)
	if err != nil {
		fmt.Printf("Error writing Markdown file for %s: %v\n", htmlFile, err)
		return
	}
}
