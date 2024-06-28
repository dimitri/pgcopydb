# check that docs are uptodate

OK = "Docs are up to date"
KO = "Docs are not up to date, please run 'make update-docs'"

all:
	git init --initial-branch=main .
	git config user.email ci@pgcopydb.org
	git config user.name "CI"
	git add docs
	git commit -m "docs before update"
	make -s -j$(nproc) update-docs
	git add docs
	git diff --staged --exit-code || (echo $(KO) && false) && echo $(OK)
