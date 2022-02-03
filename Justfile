install-publish-tools:
  pip install setuptools wheel twine

publish version:
  rm -rf dist
  VERSION={{version}} python setup.py sdist bdist_wheel
  source twine_creds.sh && twine upload dist/*
