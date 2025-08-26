import random

_COLORS = (
    'red', 'blue', 'green', 'yellow', 'purple', 'orange', 'pink', 'brown',
    'black', 'white', 'gray', 'silver', 'gold', 'cyan', 'magenta', 'lime',
    'navy', 'teal', 'olive', 'maroon', 'crimson', 'indigo', 'violet', 'coral'
)

_ADJECTIVES = (
    'swift', 'brave', 'quiet', 'bright', 'clever', 'gentle', 'bold', 'wise',
    'fierce', 'calm', 'agile', 'strong', 'quick', 'silent', 'mighty', 'graceful',
    'proud', 'noble', 'wild', 'free', 'loyal', 'keen', 'sharp', 'steady',
    'alert', 'active', 'eager', 'happy', 'lucky', 'cheerful', 'playful', 'curious'
)

_ANIMALS = (
    'wolf', 'eagle', 'lion', 'tiger', 'bear', 'fox', 'deer', 'rabbit',
    'hawk', 'owl', 'falcon', 'raven', 'shark', 'whale', 'dolphin', 'seal',
    'panda', 'koala', 'zebra', 'giraffe', 'elephant', 'rhino', 'hippo', 'cheetah',
    'leopard', 'jaguar', 'lynx', 'bobcat', 'cougar', 'panther', 'bison', 'moose',
    'elk', 'ram', 'goat', 'horse', 'stallion', 'mare', 'pony', 'mule',
    'turtle', 'lizard', 'gecko', 'iguana', 'snake', 'cobra', 'viper', 'python'
)


def generate_random_id() -> str:
    """Generate a random readable ID using optimized random selection."""
    color = random.choice(_COLORS)
    adjective = random.choice(_ADJECTIVES)
    animal = random.choice(_ANIMALS)
    return f"{color}-{adjective}-{animal}"

