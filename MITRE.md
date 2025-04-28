# Fetch latest from GitHub
git fetch origin

# Merge or rebase the branch you want (e.g. main)
git checkout main
git merge origin/main   # or `git rebase origin/main`

# Push the updated branch to GitLab
git push gitlab main
