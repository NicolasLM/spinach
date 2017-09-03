publish:
	pip install -U pip setuptools wheel twine
	python setup.py sdist
	python setup.py bdist_wheel
	twine upload dist/*
	rm -fr build dist spinach.egg-info

clean:
	rm -fr build dist spinach.egg-info

