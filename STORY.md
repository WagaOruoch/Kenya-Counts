# The Story

## Title
**"The Devolution Dividend — Has Kenya's Tax Money Worked for Its People?"**

## Tagline
*Eleven years of taxation. Forty-seven counties. One question: what did Kenyans actually get?*

---

## Narrative Structure

This project tells its story in four acts. Every chart, every table, every line of analysis must serve one of these acts. If it does not advance the narrative, it does not belong.

---

## ACT 1 — THE PROMISE (Context)
*Set the scene. Establish what was supposed to happen.*

**The opening line of the story:**
> "In 2013, Kenya made a promise to its citizens: devolve power, collect more revenue, and deliver better services to every corner of the country equally. This is the data record of whether that promise was kept."

**What this section establishes:**
- What devolution was designed to achieve
- The revenue trajectory: KRA collection targets and actuals from 2013 to 2024
- The budget growth story: total government spending over the same period
- The expectation set for citizens

**Key visual:** A single line chart — tax revenue collected, 2013–2024. Simple. Powerful. The baseline for everything that follows.

**The question this act leaves the reader with:**
*"If Kenya collected all this money — where did it go, and what did it build?"*

---

## ACT 2 — THE LEDGER (National Story)
*Follow the money at the national level.*

**The narrative thread:**
Revenue went up. The budget went up. But did spending reach the frontlines — the hospitals, the classrooms, the roads? And when it did, did outcomes improve?

**What this section shows:**
- Budget allocation by sector (healthcare, education, infrastructure) year by year
- Actual expenditure vs allocation — the compliance gap
- Debt servicing as a share of the budget: how much was consumed before it reached a citizen
- Service delivery indicators over time:
  - Healthcare: hospital beds per 1,000, maternal mortality rate, immunisation coverage
  - Education: primary school enrollment, completion rates, teacher-to-pupil ratio
  - Infrastructure: paved road network km, electrification rates

**The tension this act builds:**
Revenue grows. But debt repayment grows faster. The share of the budget that reaches frontline services tells a story of its own.

**Key visual:** A Sankey diagram — for every KSh 100 collected in tax, trace where it flows: debt repayment, recurrent expenditure, development spending, and finally, what reaches healthcare / education / infrastructure.

**The question this act leaves the reader with:**
*"If the national picture is complicated, what happens when we zoom into individual counties?"*

---

## ACT 3 — THE DIVIDE (County Story)
*Reveal that the national average hides deep inequality.*

**The narrative thread:**
Devolution promised that every Kenyan — regardless of which county they were born in — would have access to quality services. The equitable share formula was designed to correct historical imbalances. Has it worked?

**What this section shows:**
- Equitable share received per county per capita, 2013–2024
- For each county, three outcome measures:
  - Health: hospital beds per 1,000, skilled birth attendance rate
  - Education: primary completion rate, pupil-to-teacher ratio
  - Poverty: poverty headcount rate, household income levels
- A composite "service delivery score" per county combining all three
- Scatter plot: funds received vs service delivery score — who is getting value, who is not
- Choropleth map: the geography of inequality — colour-coded by outcome score

**The stories within the story:**
- The overperformers: counties doing more with less — what can others learn?
- The underperformers: counties receiving significant funds but showing poor outcomes — what is happening?
- The forgotten: counties consistently at the bottom across all three measures

**Key visual:** A choropleth map of Kenya's 47 counties, coloured by composite service delivery score. The visual should be immediately striking — the inequality should be visible before a single number is read.

**The question this act leaves the reader with:**
*"So after eleven years — what is the verdict?"*

---

## ACT 4 — THE VERDICT (Conclusion)
*Answer the question. Be honest about what the data shows and what it cannot show.*

**The narrative thread:**
The data has told its story. Now we synthesise it into findings — not political opinions, but evidence-based conclusions that any reader, regardless of their politics, must reckon with.

**What this section delivers:**
- Direct answer to the primary question: has tax money worked for Kenyans?
- The three most important findings from the national analysis
- The three most important findings from the county analysis
- What the data cannot tell us — and what further investigation would require
- What citizens, policymakers, and county governments should do with these findings

**The closing line of the story:**
> "The numbers do not lie, but they do not tell the whole story either. What they tell us is this: Kenya's tax revenue has grown every year since devolution. The services that revenue was meant to fund have not grown at the same pace, and they have not grown equally. The gap between what was promised and what was delivered is not abstract — it is measured in children who dropped out of school, in mothers who did not survive childbirth, in roads that were never built. Closing that gap begins with knowing it exists. Now we know."

---

## The Visualisation Principles

Every chart in this project must follow these rules:

1. **One chart, one insight** — the title of every chart should state the finding, not just describe the data. Not *"Tax Revenue 2013–2024"* but *"Tax Revenue Has Tripled Since Devolution"*
2. **Annotate the moments that matter** — mark 2020 (COVID), 2023 (debt crisis), 2024 (protests) on every time-series chart
3. **Always show Kenya's human scale** — where possible, translate numbers to per-capita or per-household figures. KSh 2 trillion means nothing. KSh 40,000 per Kenyan per year means something.
4. **Let outliers tell stories** — whenever a county sits far from the trend line, name it and explain it
5. **Colour with intention** — use colour to guide attention, not decorate. Red for underperformance, green for overperformance, grey for the middle.

---

## The Tone

- **Rigorous but accessible** — this is not an academic paper. A Form 4 student should be able to follow the story.
- **Evidence-led, not opinion-led** — the data makes the argument. The writer stays out of the way.
- **Respectful of complexity** — Kenya's governance challenges are real and multifaceted. The project acknowledges what data can and cannot explain.
- **Hopeful where the data allows** — where counties or programmes are working, say so clearly. This is not a cynicism project. It is an accountability project.

---

## Milestones — In Order

| Stage | Deliverable | Purpose |
|---|---|---|
| 1 | `QUESTION.md` + `STORY.md` | Define before collecting |
| 2 | Scrapers built and tested | Raw data in `/data/raw` |
| 3 | Cleaning pipeline complete | Analysis-ready data in `/data/clean` |
| 4 | `01_eda_national.ipynb` | Understand the national dataset |
| 5 | `02_eda_counties.ipynb` | Understand the county dataset |
| 6 | `03_analysis.ipynb` | Answer the sub-questions with evidence |
| 7 | `04_story.ipynb` | Build the final visualisations |
| 8 | Streamlit dashboard | Interactive version of the story |
| 9 | Written report PDF | Final narrative document |
| 10 | README.md | Public-facing project summary |
| 11 | CI/CD pipeline | Scheduled data refresh via GitHub Actions |

---

## A Note to Anyone Reading This Code

This project was built with a question before a single line of code was written. Every scraper, every cleaning function, every chart exists to serve the story above. If you are contributing to or extending this project, start here — not in the notebooks, not in the scrapers. Start with the question. Start with the story.
