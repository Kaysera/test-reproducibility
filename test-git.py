from git import Repo

repo = Repo('.')
o = repo.remotes.origin
o.pull()
print(repo)

commit_id = '0ff56046024f3b0ee3995a1637770d178e30c052'
c = repo.commit(commit_id)

for file in c.stats.files:
    print(file)

raise ValueError