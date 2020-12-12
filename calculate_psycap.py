# import statements
import pandas as pd
import nltk


# import wordlist about common misspellings
misspelling_path = "/Users/philippsach/Documents/Uni/Masterarbeit/Python_Analysis/data/word_lists/common_misspellings.csv"
df_misspellings = pd.read_csv(misspelling_path)
list_misspellings = df_misspellings["Misspelling"].to_list()

# TODO: Define creator names in user settings and not in each script separately (is done here and in calc_comm_stats)
creator_names = ["Projektgründer", "ProjektgründerSuperbacker"]
# creator_names = ["Creator", "CreatorSuperbacker"]


# definition of the word lists

list_organizational_optimism = ["aspire", "aspirer", "aspires", "aspiring", "aspiringly",
                                "assurance", "assured", "assuredly", "assuredness", "assuring",
                                "auspicious", "auspiciously", "auspiciousness", "bank on", "beamish",
                                "believe", "believed", "believes", "believing", "bullish",
                                "bullishly", "bullishness", "confidence", "confident", "confidently",
                                "encourage", "encouraged", "encourages", "encouraging", "encouragingly",
                                "ensuring", "expectancy", "expectant", "expectation", "expectations",
                                "expected", "expecting", "faith", "good omen", "hearten", "heartened",
                                "heartener", "heartening", "hearteningly", "heartens", "hope", "hoped",
                                "hopeful", "hopefully", "hopefulness", "hoper", "hopes", "hoping",
                                "ideal", "idealist", "idealistic", "idealistically", "ideally",
                                "looking up", "looks up", "optimism", "optimist", "optimistic",
                                "optimistical", "optimistically", "outlook", "positive", "positively",
                                "positiveness", "positivity", "promising", "propitious", "propitiously",
                                "propitiousness", "reassure", "reassured", "reassures", "reassuring",
                                "roseate", "rosy", "sanguine", "sanguinely", "sanguineness", "sanguinity",
                                "sunniness", "sunny"]

list_organizational_hope = ["accomplishments", "achievements", "approach", "aspiration", "aspire",
                            "aspired", "aspirer", "aspires", "aspiring", "aspiringly",
                            "assurance", "assurances", "assure", "assured", "assuredly",
                            "assuredness", "assuring", "assuringly", "assuringness", "belief",
                            "believe", "believed", "believes", "believing", "breakthrough",
                            "certain", "certainly", "certainty", "committed", "concept",
                            "confidence", "confident", "confidently", "convinced", "dare say",
                            "deduce", "deduced", "deduces", "deducing", "desire",
                            "desired", "desires", "desiring", "doubt not", "energy",
                            "engage", "engagement", "expectancy", "faith", "foresaw",
                            "foresee", "foreseeing", "foreseen", "foresees", "goal",
                            "goals", "hearten", "heartened", "heartening", "hearteningly",
                            "heartens", "hope", "hoped", "hopeful", "hopefully",
                            "hopefulness", "hoper", "hopes", "hoping", "idea",
                            "innovation", "innovative", "ongoing", "opportunity", "promise",
                            "promising", "propitious", "propitiously", "propitiousness", "solution",
                            "solutions", "upbeat", "wishes", "wishing", "yearn",
                            "yearn for", "yearning", "yearning for", "yearns for"]

list_organizational_resilience = ["adamant", "adamantly", "assiduous", "assiduously", "assiduousness",
                                  "backbone", "bandwidth", "bears up", "bounce", "bounced",
                                  "bounces", "bouncing", "buoyant", "commitment", "commitments",
                                  "committed", "consistent", "determination", "determined", "determinedly",
                                  "determinedness", "devoted", "devotedly", "devotedness", "devotion",
                                  "die trying", "died trying", "dies trying", "disciplined", "dogged",
                                  "doggedly", "doggedness", "drudge", "drudged", "drudges",
                                  "endurance", "endure", "endured", "endures", "enduring",
                                  "grit", "hammer away", "hammered away", "hammering away", "hammers away",
                                  "held fast", "held good", "held up", "hold fast", "holding fast",
                                  "holding up", "holds fast", "holds good", "immovability", "immovable",
                                  "immovably", "indefatigable", "indefatigableness", "indefatigably", "indestructibility",
                                  "indestructible", "indestructibleness", "indestructibly", "intransigence", "intransigency",
                                  "intransigent", "keep at", "keep going", "keep on", "keeping at",
                                  "keeping going", "keeping on", "keeps at", "keeps going", "keeps on",
                                  "kept at", "kept going", "kept on", "labored", "laboring",
                                  "never-tiring", "never-wearying", "perdure", "perdured", "perduring",
                                  "perseverance", "persevere", "persevered", "persevering", "persist",
                                  "persisted", "persistence", "persistent", "persisting", "pertinacious",
                                  "pertinaciously", "pertinacity", "rebound", "rebounded", "rebounding",
                                  "rebounds", "relentlessness", "remain", "remained", "remaining",
                                  "remains", "resilience", "resiliency", "resilient", "resolute",
                                  "resolutely", "resoluteness", "resolve", "resolved", "resolves",
                                  "resolving", "robust", "sedulity", "sedulous", "sedulously",
                                  "sedulousness", "snap back", "snapped back", "snapping back", "snaps back",
                                  "spring back", "springing back", "springs", "springs back", "sprung back",
                                  "stalwart", "stalwartly", "stalwartness", "stand fast", "stand firm",
                                  "standing fast", "standing firm", "stands fast", "stands firm", "stay",
                                  "steadfast", "steadfastly", "steadfastness", "stood fast", "stood firm",
                                  "strove", "survive", "surviving", "surviving", "tenacious",
                                  "tenaciously", "tenaciousness", "tenacity", "tough", "uncompromising",
                                  "uncompromisingly", "uncompromisingness", "unfaltering", "unfalteringly", "unflagging",
                                  "unrelenting", "unrelentingly", "unrelentingness", "unshakable", "unshakablely",
                                  "unshakeable", "unshaken", "unshaking", "unswervable", "unswerved",
                                  "unswerving", "unswervingly", "unswervingness", "untiring", "unwavered",
                                  "unwavering", "unweariedness", "unyielding", "unyieldingly", "unyieldingness",
                                  "upheld", "uphold", "upholding", "upholds", "zeal",
                                  "zealous", "zealously", "zealousness"]

list_organizational_confidence = ["ability", "accomplish", "accomplished", "accomplishes", "accomplishing",
                                  "accomplishments", "achievements", "achieving", "adept", "adeptly",
                                  "adeptness", "adroitly", "adroitness", "all-in", "aplomb",
                                  "arrogance", "arrogant", "arrogantly", "assurance", "assured",
                                  "assuredly", "assuredness", "backbone", "bandwidth", "belief",
                                  "capable", "capableness", "capably", "certain", "certainly",
                                  "certainness", "certainty", "certitude", "cocksurely", "cocksureness",
                                  "cocky", "commitment", "commitments", "committed", "compelling",
                                  "competence", "competency", "competent", "competently", "confidence",
                                  "confident", "confidently", "conviction", "effective", "effectively",
                                  "effectiveness", "effectual", "effectually", "effectualness", "efficacious",
                                  "efficaciously", "efficaciousness", "efficacy", "equanimity", "equanimous",
                                  "equanimously", "expertise", "expertly", "fortitude", "fortitudinous",
                                  "forward", "forwardness", "know-how", "knowledgability", "knowledgeable",
                                  "knowledgably", "masterful", "masterfully", "masterfulness", "masterly",
                                  "mastery", "overconfidence", "overconfident", "overconfidently", "persuasion",
                                  "power", "powerful", "powerfully", "powerfulness", "prevailed",
                                  "prevailing", "prevails", "prevalence", "prevalent", "reassurance",
                                  "reassure", "reassured", "reassures", "reassuring", "self-assurance",
                                  "self-assured", "self-assuring", "self-confidence", "self-confident", "self-dependence",
                                  "self-dependent", "self-reliance", "self-reliant", "stamina", "steadily",
                                  "steadiness", "steady", "strength", "strong", "stronger",
                                  "strongish", "strongly", "strongness", "superior", "superiority",
                                  "sure", "surely", "sureness", "unblinking", "unblinkingly",
                                  "undoubtedly", "undoubting", "unflappability", "unflappable", "unflinching",
                                  "unflinchingly", "unhesitating", "unhesitatingly", "unwavering", "unwaveringly"]


# obtain search patterns for regex expressions
# (?i) to ignore case of letters
# (?<!\S) to do exact matching, so that "aspired" is not matched by a search for "aspire"

string_organizational_optimism = "|".join(list_organizational_optimism)
search_organizational_optimism = r"(?i)(?<!\S)({})(?!\S)".format(string_organizational_optimism)

string_organizational_hope = "|".join(list_organizational_hope)
search_organizational_hope = r"(?i)(?<!\S)({})(?!\S)".format(string_organizational_hope)

string_organizational_resilience = "|".join(list_organizational_resilience)
search_organizational_resilience = r"(?i)(?<!\S)({})(?!\S)".format(string_organizational_resilience)

string_organizational_confidence = "|".join(list_organizational_confidence)
search_organizational_confidence = r"(?i)(?<!\S)({})(?!\S)".format(string_organizational_confidence)

string_misspellings = "|".join(list_misspellings)
search_misspellings = r"(?i)(?<!\S)({})(?!\S)".format(string_misspellings)

#print(string_organizational_optimism)
#print(search_organizational_optimism)


# define a function that counts the occurence of that
def count_words_in_list(loc_comments_df):
    
    # rule out that there is no empty content that cannot be handled
    loc_comments_df["content"] = loc_comments_df["content"].astype("string")
    loc_comments_df["content"] = loc_comments_df["content"].fillna("")
        

    loc_comments_df["count_organizational_optimism"] = loc_comments_df.content.str.count(search_organizational_optimism)
    loc_comments_df["count_organizational_hope"] = loc_comments_df.content.str.count(search_organizational_hope)
    loc_comments_df["count_organizational_resilience"] = loc_comments_df.content.str.count(search_organizational_resilience)
    loc_comments_df["count_organizational_confidence"] = loc_comments_df.content.str.count(search_organizational_confidence)
    
    loc_comments_df["count_misspellings"] = loc_comments_df.content.str.count(search_misspellings)
    
    loc_comments_df["count_psycap"] = (
        loc_comments_df["count_organizational_optimism"] +
        loc_comments_df["count_organizational_hope"] +
        loc_comments_df["count_organizational_resilience"] +
        loc_comments_df["count_organizational_confidence"]
    )
    

    loc_comments_df["commentLength"] = loc_comments_df.apply(lambda x: len(x["content"].split()), axis=1)

    return loc_comments_df


def wrapper_wordcount(loc_comments_df):
    psycap_comments_df = loc_comments_df.copy()
    psycap_comments_df = psycap_comments_df[psycap_comments_df["title"].isin(creator_names)]
    psycap_comments_df = count_words_in_list(loc_comments_df=loc_comments_df)
    psycap_comments_df = psycap_comments_df.groupby("projectID").sum()
    
    psycap_comments_df = psycap_comments_df.loc[:, "count_organizational_optimism": "commentLength"]
    psycap_comments_df["shareOptimism"] = psycap_comments_df["count_organizational_optimism"] / psycap_comments_df[
        "commentLength"]
    psycap_comments_df["shareHope"] = psycap_comments_df["count_organizational_hope"] / psycap_comments_df[
        "commentLength"]
    psycap_comments_df["shareResilience"] = psycap_comments_df["count_organizational_resilience"] / psycap_comments_df[
        "commentLength"]
    psycap_comments_df["shareConfidence"] = psycap_comments_df["count_organizational_confidence"] / psycap_comments_df[
        "commentLength"]
    psycap_comments_df["sharePsyCap"] = psycap_comments_df["count_psycap"] / psycap_comments_df["commentLength"]
    psycap_comments_df["shareMisspellings"] = psycap_comments_df["count_misspellings"] / psycap_comments_df[
        "commentLength"]

    return psycap_comments_df


if __name__ == "__main__":
    # import comments_df and filter to those comments that should be analysed - here: the comments of the creator
    comments_df = pd.read_csv("/Users/philippsach/Documents/Uni/Masterarbeit/Datasets/test/full_comments_df_art_test.csv")
    psycap_comments_df = comments_df[comments_df["title"].isin(creator_names)].copy()

    psycap_comments_df = count_words_in_list(loc_comments_df=psycap_comments_df)
    

    psycap_comments_df = psycap_comments_df.groupby("projectID").sum()
    psycap_comments_df = psycap_comments_df.loc[:, "count_organizational_optimism": "commentLength"]
    psycap_comments_df["shareOptimism"] = psycap_comments_df["count_organizational_optimism"] / psycap_comments_df[
        "commentLength"]
    psycap_comments_df["shareHope"] = psycap_comments_df["count_organizational_hope"] / psycap_comments_df[
        "commentLength"]
    psycap_comments_df["shareResilience"] = psycap_comments_df["count_organizational_resilience"] / psycap_comments_df[
        "commentLength"]
    psycap_comments_df["shareConfidence"] = psycap_comments_df["count_organizational_confidence"] / psycap_comments_df[
        "commentLength"]
    psycap_comments_df["sharePsyCap"] = psycap_comments_df["count_psycap"] / psycap_comments_df["commentLength"]
    psycap_comments_df["shareMisspellings"] = psycap_comments_df["count_misspellings"] / psycap_comments_df[
        "commentLength"]
    
    psycap_statistics = wrapper_wordcount(comments_df)



# it also counts occurences of e.g. "aspired" even though this is not in the word list
# test_df = pd.DataFrame([
#     "hello Aspired world Aspire",
#     "What is wrong with you?",
#     "I Reassure you that I am Positive"
#     ],
#     columns = ["content"]
# )

# test_df["count_organizational_optimism"] = test_df.content.str.count(search_organizational_optimism)
# test_df["count_organizational_hope"] = test_df.content.str.count(search_organizational_hope)
# test_df["count_organizational_resilience"] = test_df.content.str.count(search_organizational_resilience)
# test_df["count_organizational_confidence"] = test_df.content.str.count(search_organizational_confidence)

# print(test_df.content.str.count(r"(?<!\S)(Aspire|Reassure|Positive)(?!\S)"))
# print(test_df.content.str.count(search_organizational_optimism))

# print(test_df)
