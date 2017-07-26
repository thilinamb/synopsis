name=synopsis-tkde

all: $(name).pdf

$(name).pdf: $(name).tex ./figures/* references.bib
	pdflatex -interaction=batchmode $(name)
	bibtex $(name) || true
	pdflatex -interaction=batchmode $(name)
	pdflatex -interaction=nonstopmode $(name)
	rm -f $(name).{aux,bbl,bib,blg,log,toc,out}

clean:
	rm -f $(name).{aux,bbl,bib,blg,log,pdf,toc,out}
	rm -f changes-1.{aux,bbl,bib,blg,log,toc,out}
	rm -f changes-2.{aux,bbl,bib,blg,log,toc,out}
