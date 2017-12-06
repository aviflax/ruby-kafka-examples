FROM jruby:latest

# Ruby deps
COPY Gemfile ./
RUN bundle install

# app
COPY  . ./

ENTRYPOINT jruby processor.rb
