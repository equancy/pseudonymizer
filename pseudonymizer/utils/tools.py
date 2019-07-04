def batcher(seq, size):
    """Split sequence into chunks of a given size
    Parameters
    ----------
    seq: Sequence to ckunk
    size: Size of chunks

    Returns
    -------
    List of chunks
    """
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))
