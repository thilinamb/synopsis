name=synopsis-tkde

all: $(name).pdf changes-1.pdf changes-2.pdf

$(name).pdf: $(name).tex ./sections/* ./figures/* references.bib
	pdflatex -interaction=batchmode $(name)
	bibtex $(name) || true
	pdflatex -interaction=batchmode $(name)
	pdflatex -interaction=nonstopmode $(name)
	rm -f $(name).{aux,bbl,bib,blg,log,toc,out}

changes-1.pdf: changes-1.tex references.bib
	pdflatex -interaction=batchmode changes-1
	bibtex changes-1 || true
	pdflatex -interaction=batchmode changes-1
	pdflatex -interaction=nonstopmode changes-1
	rm -rf changes-1.{aux,bbl,bib,blg,log,toc,out}

changes-2.pdf: changes-2.tex
	pdflatex -interaction=nonstopmode changes-2
	rm -rf changes-2.{aux,bbl,bib,blg,log,toc,out}

clean:
	rm -f $(name).{aux,bbl,bib,blg,log,pdf,toc,out}
	rm -f changes-1.{aux,bbl,bib,blg,log,toc,out}
	rm -f changes-2.{aux,bbl,bib,blg,log,toc,out}
