# D2E Plugins


## How to modify package.json for local development

In the flow to be modified in trex > flow > flows

1. Change the value of the image to `ph/flow:latest`
2. Run `yarn build` to build `ph/flow:latest` after making changes to flow code
3. In d2e-plugins repo, set PLUGINS_SEED_UDPATE=True and then run `yarn start:minerva trex` to reinitialize the plugins
