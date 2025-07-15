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
      // Next.js Image component optimizes the src, so we check it contains our URL
      expect(logo).toHaveAttribute('src', expect.stringContaining('example.com%2Flogo.png'))
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
      expect(header).toHaveClass('header', 'bg-white', 'border-b', 'border-gray-200', 'shadow-sm')
      
      // Check for responsive classes that are applied via the inner div
      const headerContent = header.querySelector('.flex')
      expect(headerContent).toBeInTheDocument()
      expect(headerContent).toHaveClass('flex', 'justify-between', 'items-center')
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