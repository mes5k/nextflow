
make compile
make pack

docker build -f Dockerfile.mike -t nextflow-base:latest .
