GHCPKGDB ?= ${shell find .cabal-sandbox -name *-packages.conf.d -type d -print}

GHCFLAGS := -package-db=$(GHCPKGDB) $(GHCFLAGS)

SOURCES=$(wildcard *.hs)
TARGETS=$(patsubst %.hs,%,$(notdir $(SOURCES)))

RUNPHONYTARGETS=$(patsubst %.hs,%.run,$(TARGETS))
CHECKPHONYTARGETS=$(patsubst %.hs,%.check,$(TARGETS))

.PHONY: $(RUNPHONYTARGETS) $(CHECKPHONYTARGETS) clean clobber

$(TARGETS):
	ghc $(GHCFLAGS) $@.hs

%.run: % clean
	./$(patsubst %.run,%,$@) +RTS -s

%.check: GHCFLAGS = -package-db=$(GHCPKGDB) -fno-code
%.check: %
	:

clean:
	rm -f *.hi *.o

clobber:
	rm -f *.hi *.o
