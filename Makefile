.DEFAULT_GOAL := build-all

build-all: module

module:
	$(MAKE) -C ./src

clean:
	$(MAKE) -C ./src clean
