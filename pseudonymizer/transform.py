import hmac
import hashlib
import logging
import numpy as np

from pseudonymizer.config.main import SALT_KEY

logger = logging.getLogger(__name__)


def pseudonymize(value, salt=SALT_KEY):
    """Pseudonymize value with salt, using HMAC-SHA256 encoding

    Parameters
    ----------
    value: value to be pseudonymized
    salt: hazard salt for additional protection

    Returns
    -------
    pseudonymized value using HMAC-SHA256
    """

    # NOTE: Here we must bypass empty or None value as
    # it will introduce specific hash value
    if value is None or value is np.nan or value == '':
        return None

    return hmac.new(
        key=salt.encode('utf-8'),         # La clé
        msg=str(value).encode('utf-8'),   # La donnée à pseudonymiser
        digestmod=hashlib.sha256          # La fonction de hash
    ).hexdigest()                         # L’encodage en hexadécimal