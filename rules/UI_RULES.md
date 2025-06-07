# UI Design Principles

## Core Values

- **Clarity First**: Every UI element should have a clear purpose and communicate value instantly
- **Progressive Disclosure**: Show essential information first, details on demand
- **Visual Hierarchy**: Guide users through content with thoughtful layout and typography
- **White Space**: Use generous spacing to improve readability and reduce cognitive load
- **Consistency**: Maintain uniform styling, patterns and interactions throughout

## Design Guidelines

### Layout & Structure
- Use a clean grid system with clear visual hierarchy
- Maintain comfortable margins and padding (min 16px between major sections)
- Limit content width to improve readability (~800px max for text)
- Group related information into logical sections
- Use cards, panels, or containers to visually segment content

### Typography
- Use a clear, modern sans-serif font stack
- Maintain readable font sizes (min 16px body text)
- Use font weights and sizes to establish hierarchy
- Limit to 2-3 font styles maximum
- Ensure sufficient contrast ratios for accessibility

### Color & Visual Design  
- Use a restrained color palette (2-3 primary colors + accents)
- Reserve bright colors for important actions and alerts
- Use subtle shadows and borders to create depth
- Maintain adequate color contrast (WCAG AA standards)
- Use color meaningfully, not decoratively

### Components & Interactions
- Make primary actions prominent and obvious
- Use familiar UI patterns and components
- Provide clear visual feedback on interactions
- Keep forms simple and focused
- Use progressive disclosure for complex workflows

### Data Visualization
- Choose appropriate chart types for the data
- Label axes and data points clearly
- Use color strategically to highlight insights
- Include clear titles and legends
- Maintain a clean, uncluttered look

## Value Communication

### Metrics & KPIs
- Display key metrics prominently
- Use visual indicators for status/progress
- Compare metrics to benchmarks where relevant
- Show trends over time when applicable
- Highlight improvements and positive changes

### AI Insights
- Clearly separate AI analysis from raw data
- Show confidence levels and reasoning
- Use progressive disclosure for detailed explanations
- Highlight actionable recommendations
- Provide context for AI decisions

### Business Impact
- Lead with business value and outcomes
- Show before/after comparisons
- Highlight time/cost savings
- Use real numbers and concrete examples
- Make ROI visible and obvious

## Implementation Notes

- Use Streamlit's native components where possible
- Maintain consistent spacing with st.columns
- Leverage custom HTML/CSS sparingly and purposefully
- Test across different screen sizes
- Consider load times and performance
