import random
import re
from re import Pattern

GOODBYE_INPUTS: set[str] = {"bye", "goodbye", "exit", "quit"}
GOODBYE_RESPONSES: list[str] = [
    "Goodbye. It was nice talking to you.",
    "Take care!",
    "Farewell!",
]

INTRO_RESPONSES: list[str] = [
    "Hello %s, I'm Eliza.",
    "Nice to meet you, %s.",
]

ELIZA_FACTS: list[str] = [
    "Did you know ELIZA was created in the 1960s?",
    "I'm named after Eliza Doolittle from 'Pygmalion'.",
]

REFLECTED_WORDS: dict[str, str] = {
    "am": "are",
    "was": "were",
    "i": "you",
    "i'd": "you would",
    "i've": "you have",
    "i'll": "you will",
    "my": "your",
    "you": "me",
    "are": "am",
    "you're": "I am",
    "you've": "I have",
    "you'll": "I will",
    "your": "my",
    "yours": "mine",
    "me": "you",
}

REQUEST_INPUT_REGEX_TO_RESPONSE_OPTIONS: dict[Pattern[str], list[str]] = {
    re.compile(r"i need (.*)"): [
        "Why do you need %s?",
        "Would it really help you to get %s?",
        "Are you sure you need %s?",
    ],
    re.compile(r"why don'?t you ([^\?]*)\??"): [
        "Do you really think I don't %s?",
        "Perhaps eventually I will %s.",
        "Do you really want me to %s?",
    ],
}

DEFAULT_RESPONSES: list[str] = [
    "Please tell me more.",
    "Let's change focus a bit… Tell me about your family.",
    "Can you elaborate on that?",
]


def reply(user_input: str) -> tuple[str, bool]:
    """Respond to *user_input* in the style of a psychotherapist.

    Parameters
    ----------
    user_input :
        The raw string entered by the user.

    Returns
    -------
    tuple[str, bool]
        - The response text.
        - ``True`` if the conversation should end (i.e., user said goodbye);
          otherwise ``False``.

    """
    cleaned_input = _preprocess(user_input)
    if cleaned_input in GOODBYE_INPUTS:
        return _random_element(GOODBYE_RESPONSES), True

    return _lookup_response(cleaned_input), False


def get_intro_responses(name: str) -> list[str]:
    """Generate introductory lines tailored to *name*.

    Parameters
    ----------
    name :
        The conversation partner’s name.

    Returns
    -------
    list[str]
        A list of sentences Eliza might say at the start of a chat.

    """
    intros = [template % name for template in INTRO_RESPONSES]
    intros.append(_random_element(ELIZA_FACTS))
    intros.append("How are you feeling today?")
    return intros


def _lookup_response(cleaned_input: str) -> str:
    for pattern, responses in REQUEST_INPUT_REGEX_TO_RESPONSE_OPTIONS.items():
        match = pattern.search(cleaned_input)
        if not match:
            continue

        response = _random_element(responses)
        if "%s" not in response:
            return response

        fragment = _reflect(match.group(1))
        return response % fragment

    return _random_element(DEFAULT_RESPONSES)


def _preprocess(user_input: str) -> str:
    """Lower-case & trim punctuation/whitespace."""
    return user_input.strip().lower().strip(".!?\"'")


def _reflect(fragment: str) -> str:
    """Swap first/second-person pronouns in *fragment*.

    Example:
    -------
    "I am happy" -> "you are happy"

    """
    words = fragment.split()
    reflected = [REFLECTED_WORDS.get(word, word) for word in words]
    return " ".join(reflected)


def _random_element(options: list[str]) -> str:
    """Return a random element from *options* (wrapper for :pyfunc:`random.choice`)."""
    return random.choice(options)
