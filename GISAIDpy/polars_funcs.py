"""
File containing functions pertaining to polars implementation of fasta reading.
"""

import polars as pl

def read_fasta(file=None, get_sequence=True):
    fasta_lines = pl.read_csv(file, sep='\n').to_list("a").flatten()

    # Get sequence names
    seq_name_index = [i for i, line in enumerate(fasta_lines) if line.startswith(">")]
    strain = [line[1:] for line in fasta_lines[seq_name_index]]

    if get_sequence:
        # Get sequence
        seq_aa_start_index = [i + 1 for i in seq_name_index]
        seq_aa_end_index = seq_name_index[1:] + [len(fasta_lines)]
        sequence = ["".join(fasta_lines[start:end]).replace(" ", "") for start, end in zip(seq_aa_start_index, seq_aa_end_index)]

        return pl.DataFrame({
            "strain": strain,
            "sequence": sequence
        })
    else:
        return pl.DataFrame({
            "strain": strain
        })

