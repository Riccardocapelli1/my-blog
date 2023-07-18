## List of sources and commands

### Sources
1. setup github action to deploy hugo https://theplaybook.dev/docs/deploy-hugo-to-github-pages/
2. setup papermod theme https://adityatelange.github.io/hugo-PaperMod/posts/papermod/papermod-installation/
3. setup hugo with docker: https://dev.to/robinvanderknaap/setup-hugo-using-docker-43pm

### Docker/Git cmd 

docker run --rm klakegg/hugo:0.107.0-ext-alpine version

docker run --rm -v C:/Projects/hugo/my_blog/:/src/ klakegg/hugo:0.100.2-ext-alpine new site my_blog --format yaml

docker run --rm -p 1313:1313 -v C:/Projects/hugo/my_blog/:/src/ klakegg/hugo:0.100.2-ext-alpine server

git init
git clone https://github.com/adityatelange/hugo-PaperMod themes/PaperMod --depth=1
git submodule add --depth=1 https://github.com/adityatelange/hugo-PaperMod.git themes/PaperMod

docker run --rm -v C:/Projects/hugo/my_blog/:/src/ klakegg/hugo:0.100.2-ext-alpine new --kind post docs/MsFabric1.md

docker ps
docker exec -it 3bf51a685d2e bash
hugo new post MsFabric2.md
hugo new --kind post fabric3.md
