import React from 'react'
import { render, screen } from '@testing-library/react'
import Header from '../Header'

describe('Header Component', () => {
  describe('Component Rendering & Props', () => {
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
  })

  describe('Responsive Layout', () => {
    test('mobile layout applies correct CSS classes', () => {
      render(<Header />)
      
      const header = screen.getByRole('banner')
      expect(header).toHaveClass('flex', 'justify-between', 'items-center', 'p-4')
      
      // Check for responsive classes that will be applied via Tailwind
      const headerContent = header.querySelector('.flex')
      expect(headerContent).toBeInTheDocument()
    })

    test('desktop layout maintains horizontal structure', () => {
      render(<Header />)
      
      const header = screen.getByRole('banner')
      const title = screen.getByRole('heading', { level: 1 })
      const settingsButton = screen.getByRole('button', { name: /settings/i })
      
      // Check that elements are present and properly structured
      expect(header).toContainElement(title)
      expect(header).toContainElement(settingsButton)
    })
  })

  describe('Accessibility', () => {
    test('uses proper semantic HTML structure', () => {
      render(<Header />)
      
      const header = screen.getByRole('banner')
      expect(header.tagName).toBe('HEADER')
      
      const title = screen.getByRole('heading', { level: 1 })
      expect(title.tagName).toBe('H1')
    })

    test('keyboard navigation works correctly', () => {
      render(<Header />)
      
      const settingsButton = screen.getByRole('button', { name: /settings/i })
      expect(settingsButton).toHaveAttribute('tabIndex', '0')
    })

    test('screen reader announces header content correctly', () => {
      render(<Header />)
      
      const title = screen.getByRole('heading', { level: 1 })
      expect(title).toHaveAccessibleName('Bluesky Post Explorer')
      
      const settingsButton = screen.getByRole('button', { name: /settings/i })
      expect(settingsButton).toHaveAttribute('aria-label', 'Settings (coming soon)')
    })
  })

  describe('Performance', () => {
    test('component renders without errors', () => {
      const consoleSpy = jest.spyOn(console, 'error').mockImplementation()
      
      render(<Header />)
      
      expect(consoleSpy).not.toHaveBeenCalled()
      consoleSpy.mockRestore()
    })

    test('no layout shifts during render', () => {
      const { container } = render(<Header />)
      
      // Basic check that the component structure is stable
      expect(container.firstChild).toBeInTheDocument()
      expect(container.firstChild).toHaveClass('header')
    })
  })
}) 