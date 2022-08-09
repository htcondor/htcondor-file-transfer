FROM	debian:latest

RUN	apt-get -qq update &&\
	apt-get install --no-install-recommends -y ca-certificates host curl git &&\
	apt-get clean &&\
	rm -rf /var/lib/apt/lists/*

RUN	useradd eht
USER	eht
WORKDIR	/home/eht

RUN	git clone --depth=1 https://github.com/brianaydemir/htcondor_file_transfer_ep.git
WORKDIR	htcondor_file_transfer_ep

RUN	./setup.sh -c ap7.ospool.osg-htc.org -n eht_illinois_transfer -u ckc
