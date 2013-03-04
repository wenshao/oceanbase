test.dir = test.output
daily-test.dir = daily-test.output
daily-test.list = tr1.single-cluster:obi=ob1 tr2.two-cluster:obi=ob4
daily-profile.list = dev.load dev.fail perf.load perf.fail comp1.tps comp5.tps comp10.tps
dev.test.list = tr5.dev:obi=ob7:write=100:scan=100:mget=100
perf.test.list = tr5.perf:obi=ob8:write=100:scan=100:mget=100
comp2.test.list = tr5.cond-base:obi=ob5:write_size=2:cond_size=0 tr5.cond:obi=ob6:write_size=2:cond_size=1
comp4.test.list = tr5.cond-base:obi=ob5:write_size=4:cond_size=0 tr5.cond:obi=ob6:write_size=4:cond_size=2
comp8.test.list = tr5.cond:obi=ob6:write_size=8:cond_size=2 tr5.cond-nocond:obi=ob6:write_size=8:cond_size=0
newcond.test.list = tr5.cond:obi=ob6:write_size=8:cond_size=2

help:
	@echo -e 'make daily-test.redo'
	@echo -e 'make $(test.dir)/{dev,perf}.{load,fail}.test.png'
	@echo -e 'make $(test.dir)/comp{1,5,10}.tps.test.png'
all: tools/iof
	make -C mixed_test
	make -C syschecker
	make -C bigquery
	make -C sqltest
clean:
	make clean -C mixed_test
	make clean -C syschecker
	make clean -C bigquery
	make clean -C sqltest

iof-dir ?= ../io_fault
iof-src = $(wildcard $(iof-dir)/*.c)
tools/iof: $(iof-src)
	mkdir -p tools && make iof -C $(iof-dir) && cp $(iof-dir)/iof tools

%.rm:
	rm -rf $(dir $*)
%.mkdir:
	mkdir -p $(dir $*)
daily-test.redo: $(daily-test.dir)/.rm daily-test daily-profile
daily-test:
	make $(patsubst %,$(daily-test.dir)/%.test.output,$(daily-test.list))
daily-profile:
	make $(patsubst %,$(daily-test.dir)/%.test.png,$(daily-profile.list))
%.client.output: %.mkdir
	./deploy.py $(basename $(notdir $*)).run $(suffix $*) target=$@
%.test.output: %.mkdir
	./deploy.py $(basename $(notdir $*)).run $(suffix $*) 2>&1 |tee $@
%.test.png: target=$(basename $(notdir $*))
%.test.png: type=$(patsubst .%,%,$(suffix $(notdir $*)))
%.test.png: deps=$(patsubst %,$(dir $*)/%.client.output,$($(target).test.list))
%.test.png:
	make $(deps) && ./plot.py $(type) $@ $(deps)
