setup-linux:
	# Create python virtualenv & source it
	python3 -m venv .venv
	source .venv/bin/activate
	

install:
	# This should be run from inside a virtualenv
	git clone https://github.com/chriotte/python-powerservice.git
	pip install --upgrade pip &&\
		pip install -r requirements.txt 
	pip install ./python-powerservice

all: setup-linux install
