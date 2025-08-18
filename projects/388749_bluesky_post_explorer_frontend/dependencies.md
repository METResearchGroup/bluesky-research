# Dependencies - Bluesky Post Explorer Frontend

## Internal Dependencies

| Dependency | Type | Owner | Status | Notes |
|------------|------|-------|--------|-------|
| Project Foundation Setup (MET-10) | Blocking | Development Team | Planned | Required for all subsequent tasks |
| Search Form Implementation (MET-11) | Blocking | Development Team | Planned | Required for MET-12 |
| Results Table Component (MET-12) | Blocking | Development Team | Planned | Required for MET-13 |

## External Dependencies

| Dependency | Type | Owner | Status | Risk Level | Mitigation |
|------------|------|-------|--------|------------|------------|
| Design Assets (Logo/Branding) | Optional | Design Team | Pending | Low | Use placeholder assets initially |
| API Specification | Optional | Backend Team | Pending | Medium | Use mock data for development |
| Content Guidelines | Optional | Content Team | Pending | Low | Use placeholder copy with approval |

## Technology Dependencies

| Dependency | Version | Purpose | Installation Status |
|------------|---------|---------|-------------------|
| Next.js | 14.x | Framework | Pending |
| TypeScript | 5.x | Type Safety | Pending |
| Tailwind CSS | 3.x | Styling | Pending |
| React Hook Form | Latest | Form Management | Pending |
| Headless UI | Latest | UI Components | Pending |
| Jest | Latest | Testing | Pending |
| React Testing Library | Latest | Component Testing | Pending |

## Service Dependencies

| Service | Purpose | Status | Backup Plan |
|---------|---------|--------|-------------|
| Vercel | Deployment | Available | Alternative: Netlify |
| NPM Registry | Package Management | Available | Alternative: Yarn |
| GitHub | Source Control | Available | N/A |
| Linear API | Project Management | Available | Manual tracking |

## RACI Matrix

| Dependency | Responsible | Accountable | Consulted | Informed |
|------------|-------------|-------------|-----------|----------|
| Project Foundation | Development Team | Technical Lead | Designer | Product Manager |
| API Integration | Backend Team | Technical Lead | Frontend Team | Product Manager |
| Design Assets | Design Team | Design Lead | Development Team | Product Manager |
| Content Guidelines | Content Team | Content Lead | Development Team | Product Manager |

## Dependency Timeline

- **Week 1:** Complete MET-10 (Project Foundation)
- **Week 2:** Complete MET-11 (Search Form) - depends on MET-10
- **Week 3:** Complete MET-12 (Results Table) - depends on MET-11  
- **Week 4:** Complete MET-13, MET-14, MET-15 - depends on previous work

---

*This file will be updated as dependencies are resolved or new ones are identified.* 