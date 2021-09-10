version=0.1
commit_count=$(shell git rev-list --count HEAD)

git-tag:
	git tag "v$(version).$(commit_count)"

git-push-tag:
	git push origin --tags

test:
	pytest -v --log-level=INFO
