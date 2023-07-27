'''
to do proper word count, we need to expand abbreviations

'''


# add more abbreviations and their full forms here:
abbreviations = {
    "akw": "atomkraftwerk",
    "akws": "atomkraftwerke",
    "mwst": "mehrwertsteuer",
}


def expand_abbreviations(tokens):
    for i in range(len(tokens)):
        if tokens[i] in abbreviations:
            tokens[i] = abbreviations[tokens[i]]
    return tokens


if __name__ == "__main__":
    print(expand_abbreviations(["akw", "akws", "energie", "gas", "heizen", "heizung", "energiesparen", "sparen", "spar", "sparsam", "spart"]))