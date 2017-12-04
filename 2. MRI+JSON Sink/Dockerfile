FROM ruby:2.4

# deps
COPY Gemfile ./
RUN bundle install

# app
COPY  . ./

ENTRYPOINT ruby sink.rb
