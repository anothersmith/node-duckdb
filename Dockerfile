FROM amazon/aws-lambda-nodejs:14
WORKDIR /app
RUN yum update -y && \
  yum -y install aws-cli && \
  yum -y install openssl11-devel && \
  yum install make -y && \
  yum install yum install gcc-c++ -y && \
  yum install cmake3 -y && \
  yum remove cmake -y && \
  ln -s /usr/bin/cmake3 /usr/bin/cmake && \
  npm install -g yarn
RUN curl -L -o /usr/local/bin/dumb-init https://github.com/Yelp/dumb-init/releases/download/v1.2.2/dumb-init_1.2.2_amd64
RUN chmod +x /usr/local/bin/dumb-init
ENTRYPOINT ["/usr/local/bin/dumb-init", "--"]
