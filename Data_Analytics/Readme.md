# NLSY Dataset Analysis

## Overview
This project explores the relationship between income and various demographic, social, and behavioral factors using the National Longitudinal Survey of Youth (NLSY) dataset. The aim is to identify the impact of factors such as criminal history, gender, race, and drug usage on income and analyze any potential discrepancies in income across different races.

## Dataset
The dataset used in this project is derived from the NLSY, which includes information collected from 9,000 individuals. Key features of the dataset include:

- **Income**: Annual income of individuals.
- **Demographic factors**:  Race.
- **Social factors**: Criminal history, drug usage, Education, Jobs.
- **Other factors**: Education level, employment status, and geographic location.

## Objectives
1. Determine the impact of demographic, social, and behavioral factors on income.
2. Identify and analyze income discrepancies based on race.
3. Generate insights to inform policies for reducing income inequality.

## Tools and Technologies
- **Programming Language**: R
- **Libraries Used**: 
  - `tidyverse` for data manipulation and visualization
  - `dplyr` for data cleaning and transformation
  - `ggplot2` for visualizations
  - `lm()` for linear regression analysis

## Methodology
1. **Data Cleaning**:
   - Removed missing or inconsistent data.
   - Standardized categorical variables (e.g., race, gender).

2. **Exploratory Data Analysis (EDA)**:
   - Visualized income distributions across various groups.
   - Explored correlations between income and factors like education, gender, and criminal history.

3. **Statistical Analysis**:
   - Performed regression analysis to quantify the impact of each factor on income.
   - Tested for statistical significance of the observed discrepancies.

4. **Results Interpretation**:
   - Highlighted key insights and actionable recommendations.

## Key Findings
1. **Impact of Criminal History**:
   - Individuals with a criminal history tend to have lower average incomes compared to those without.

2. **Gender Disparity**:
   - Women, on average, earn less than men, even after controlling for other factors.

3. **Racial Discrepancies**:
   - Significant income differences were observed between races, even when other factors were held constant.

4. **Education and Income**:
   - Higher education levels strongly correlate with increased income, irrespective of race or gender.

## Visualizations
- **Income Distribution by Race**: Bar charts showing average income per racial group.
- **Impact of Education on Income**: Scatter plots and trend lines.
- **Income vs. Criminal History**: Box plots.

## Challenges
- Handling missing data without introducing bias.
- Ensuring the model accurately captures complex relationships between variables.

## Future Work
- Expand the analysis to include additional factors like geographic regions and family background.
- Apply machine learning techniques for more accurate income prediction.
- Develop policy recommendations to address observed disparities.

## How to Run the Project
1. Clone the repository:
   ```bash
   git clone <repository-url>
   ```
2. Install required R libraries:
   ```R
   install.packages(c("tidyverse", "ggplot2", "dplyr"))
   ```
3. Run the R scripts:
   ```R
   source("nlsy_analysis.R")
   ```

## Contributing
Contributions are welcome! If you have suggestions for improvement or additional analysis, feel free to submit a pull request.

## License
This project is licensed under the MIT License. See the `LICENSE` file for details.

## Contact
For questions or feedback, please contact "Hari Etta" at "hetta@hawk.iit.edu".
