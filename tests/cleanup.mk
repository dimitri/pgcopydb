# cleanup the external volume between docker-compose runs.

cleanup:
	sudo rm -rf $(TMPDIR)/pgcopydb
	sudo install -d -o docker $(TMPDIR)/pgcopydb

	sudo rm -rf $(XDG_DATA_HOME)
	sudo install -d -o docker $(XDG_DATA_HOME)
