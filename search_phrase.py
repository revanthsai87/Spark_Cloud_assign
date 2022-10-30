import Textclean as tc
import savefile


def and_phrase(d3,and_word):
    d4 = d3.filter(lambda x: x[0] == (and_word.split(" ")[0])).map(lambda x: tc.and_help(x[1])).flatMap(lambda x: x)
    for y in and_word.split(" ")[1:]:
        d5 = d3.filter(lambda x: x[0] == (y)).map(lambda x: tc.and_help(x[1])).flatMap(lambda x: x)
        d4 = d4.cogroup(d5,10)
        d4 = d4.mapValues(lambda x: tuple(map(list, x)))
        d4 = d4.mapValues(lambda x: list(set(x[0]).intersection(set(x[1])))).flatMapValues(lambda x: x)
    d5 = d4.map(lambda x: tc.get_line(x))
    savefile.save(d5, "and_search_result")
    # print(d5.collect())


def or_phrase(d3,or_word):

    d4 = d3.filter(lambda x: x[0] == (or_word.split(" ")[0])).map(lambda x: tc.and_help(x[1])).flatMap(lambda x: x)
    inter = [x for x in d4.collect()[0]]
    for y in or_word.split(" ")[1:]:
        d5 = d3.filter(lambda x: x[0] == (y)).map(lambda x: tc.and_help(x[1])).flatMap(lambda x: x)
        d4 = d4.union(d5)
    d6 = d4.groupByKey().map(lambda x: (x[0], [*set(list(x[1]))])).flatMapValues(lambda x: x)
    d6 = d6.map(lambda x: tc.get_line(x))
    savefile.save(d6, "or_search_result")
    # print(d6.collect())
