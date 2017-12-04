FROM ruby:2.4-stretch

# apt deps
RUN apt-get update -yy -qq
RUN apt-get install -yy -qq librocksdb-dev

# Ruby deps
COPY Gemfile ./
RUN bundle install

# app
COPY  . ./

ENTRYPOINT bundle exec ruby processor.rb
