name=neptune-geo

$(name).pdf: $(name).tex ./sections/* ./figures/* references.bib
	pdflatex -interaction=batchmode $(name)
	bibtex $(name) || true
	pdflatex -interaction=batchmode $(name)
	pdflatex -interaction=nonstopmode $(name)
	rm -f $(name).{aux,bbl,bib,blg,log,toc,out}

clean:
	rm -f $(name).{aux,bbl,bib,blg,log,pdf,toc,out}
