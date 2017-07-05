name=synopsis-tkde

all: $(name).pdf changes.pdf

$(name).pdf: $(name).tex ./sections/* ./figures/* references.bib
	pdflatex -interaction=batchmode $(name)
	bibtex $(name) || true
	pdflatex -interaction=batchmode $(name)
	pdflatex -interaction=nonstopmode $(name)
	rm -f $(name).{aux,bbl,bib,blg,log,toc,out}

changes.pdf: changes.tex references.bib
	pdflatex -interaction=batchmode changes
	bibtex changes || true
	pdflatex -interaction=batchmode changes
	pdflatex -interaction=nonstopmode changes
	rm -rf changes.{aux,bbl,bib,blg,log,toc,out}

clean:
	rm -f $(name).{aux,bbl,bib,blg,log,pdf,toc,out}
	rm -f changes.{aux,bbl,bib,blg,log,toc,out}
