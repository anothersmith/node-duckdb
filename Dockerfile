FROM 059037012730.dkr.ecr.us-east-1.amazonaws.com/deepcrawl/odin-api:latest
WORKDIR /app
RUN yum -y install openssl-devel
