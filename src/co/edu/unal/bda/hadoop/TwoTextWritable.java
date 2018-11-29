package co.edu.unal.bda.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang3.builder.CompareToBuilder;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TwoTextWritable implements WritableComparable<TwoTextWritable> {

	private Text letter;
	private Text word;

	public TwoTextWritable() {
		this.letter = new Text("");
		this.word = new Text("");
	}

	public void set(String letter, String word) {
		this.letter = new Text(letter);
		this.word = new Text(word);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		letter.write(out);
		word.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.letter.readFields(in);
		this.word.readFields(in);
	}

	@Override
	public int compareTo(TwoTextWritable other) {
		return new CompareToBuilder().append(this.letter, other.letter)
				.append(this.word, other.word).toComparison();
	}

	@Override
	public boolean equals(Object other) {

		if (other == this)
			return true;
		if (!(other instanceof TwoTextWritable)) {
			return false;
		}

		TwoTextWritable two = (TwoTextWritable) other;

		return new EqualsBuilder().append(letter, two.letter).append(word, two.word).isEquals();
	}

	@Override
	public int hashCode() {
		return new HashCodeBuilder(17, 37).append(letter).append(word).toHashCode();
	}

	public String toString() {
		return new ToStringBuilder(this).append("letter", letter).append("word", word).toString();
	}

}
