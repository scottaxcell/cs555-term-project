package cs555.project.drivers;

import java.io.Serializable;
import java.math.RoundingMode;
import java.text.DecimalFormat;

public class GrossStats implements Comparable, Serializable {
	
	DecimalFormat df = new DecimalFormat("#.#####");
	
	int budget;
	int revenue;

	public GrossStats() {}
	
	public GrossStats(int budget, int revenue) {
		this.budget = budget;
		this.revenue = revenue;
		df.setRoundingMode(RoundingMode.CEILING);
	}
	
    @Override
    public String toString() {
        return "GrossStats{" +
        	"budget = " + budget +
        	" revenue = " + revenue +
        	" profitRatio = " + df.format(getProfitRatio()) +
        	" gross = " + getGross() + 
        	" profitable = " + madeAProfit() +
            '}';
    }

    @Override
    public int compareTo(Object o) {
    	return Float.compare(getProfitRatio(),  ((GrossStats) o).getProfitRatio());
    }
    
    public float getProfitRatio() {
		return ((float) revenue) / budget;
    }
    
    public int getGross() {
    	return (revenue - budget);
    	}
    
    public boolean madeAProfit() {
    	return revenue > budget ? true : false;
    }
 }
