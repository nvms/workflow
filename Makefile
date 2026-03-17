.PHONY: test test-watch types types-clean clean install up down logs

test:
	npx vitest --reporter=verbose --run

test-watch:
	npx vitest

types:
	npx tsc --declaration --allowJs --emitDeclarationOnly --skipLibCheck \
		--target es2020 --module nodenext --moduleResolution nodenext \
		--strict false --esModuleInterop true --outDir ./types src/index.js src/sqliteDriver.js src/postgresDriver.js

types-clean:
	rm -rf types

clean:
	rm -rf node_modules

install:
	npm install

up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f
