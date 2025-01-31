all: mv mvp mvt

mv: mv.c
	gcc -Wall -o mv mv.c
mvp: mvp.c
	gcc -Wall -o mvp mvp.c

mvt: mvt.c
	gcc -Wall -o mvt mvt.c -lpthread

clean:
	rm -fr mv *~ split*.txt intermediate*.txt mvp-split*.txt mvp mvt mvt-split*.txt
