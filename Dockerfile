FROM elixir:1.4

RUN mix local.hex --force
RUN mix local.rebar --force

WORKDIR /app

COPY mix.exs /app/
COPY mix.lock /app/
COPY config/ /app/config/

ENV MIX_ENV test

RUN mix deps.get
RUN mix deps.compile

COPY bin/ /app/bin/
COPY lib/ /app/lib/
COPY priv/ /app/priv/
COPY test/ /app/test/
COPY codeship-services.yml /app/
COPY codeship-steps.yml /app/
COPY dialyzer.ignore-warnings /app/

RUN MIX_ENV=${MIX_ENV} mix compile

