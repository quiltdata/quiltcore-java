.PHONY: all verify clean compile

verify:
	./gradlew check || open lib/build/reports/tests/test/index.html

clean:
	./gradlew clean

compile: clean
	./gradlew compileJava
