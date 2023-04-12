all: test

init:
	pip install -r requirements.txt

install:
	#python setup.py install
	pip install -e .

sdist:
	echo 'python setup.py bdist --help-formats'
	python setup.py sdist

test:
	nosetests tests

