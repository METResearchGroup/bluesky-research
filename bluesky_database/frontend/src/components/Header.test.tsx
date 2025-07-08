import React from 'react'
import { render, screen } from '@testing-library/react'
import Header from './Header'

describe('Header Component', () => {
  test('renders with correct title "Bluesky Post Explorer"', () => {
    render(<Header />)
    
    const title = screen.getByRole('heading', { level: 1 })
    expect(title).toBeInTheDocument()
    expect(title).toHaveTextContent('Bluesky Post Explorer')
  })

  test('accepts custom logo prop', () => {
    const logoUrl = 'https://example.com/logo.png'
    render(<Header logoUrl={logoUrl} />)
    
    const logo = screen.getByRole('img')
    expect(logo).toBeInTheDocument()
    expect(logo).toHaveAttribute('src', logoUrl)
    expect(logo).toHaveAttribute('alt', 'Bluesky Post Explorer Logo')
  })

  test('settings icon placeholder renders correctly', () => {
    render(<Header />)
    
    const settingsButton = screen.getByRole('button', { name: /settings/i })
    expect(settingsButton).toBeInTheDocument()
    expect(settingsButton).toBeDisabled()
    expect(settingsButton).toHaveAttribute('aria-label', 'Settings (coming soon)')
  })

  test('uses proper semantic HTML structure', () => {
    render(<Header />)
    
    const header = screen.getByRole('banner')
    expect(header.tagName).toBe('HEADER')
    
    const title = screen.getByRole('heading', { level: 1 })
    expect(title.tagName).toBe('H1')
  })
}) 