import Textclean as tc
import savefile


def search(d3, word):
    d4 = d3.filter(lambda x: x[0] == word).flatMapValues(lambda x: x)
    d5 = d4.map(lambda x: x[1])
    d5 = d5.map(lambda x: tc.get_line(x))
    savefile.save(d5, "search_word")
    # print(d5.take(10))
    # tc.write_to_file(d5)
