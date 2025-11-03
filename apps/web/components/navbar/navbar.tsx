import { Button } from "@/components/ui/button";
import { Logo } from "./logo";
import { NavMenu } from "./nav-menu";
import { NavigationSheet } from "./navigation-sheet";
import ThemeToggle from "../theme-toggle";
import { WalletConnect } from "@/components/wallet-connect";

const Navbar = () => {
  return (
    <nav className="h-16 bg-background border-b border-accent">
      <div className="h-full flex items-center justify-between max-w-screen-xl mx-auto px-4 sm:px-6">
        <Logo />

        {/* Desktop Menu */}
        <NavMenu className="hidden md:block" />

        <div className="flex items-center gap-3">
          <ThemeToggle />
          <WalletConnect />
          <Button variant="outline" className="hidden sm:inline-flex" asChild>
            <a href="https://github.com/ResearchComputer/OpenComputeFramework" target="_blank">
              GitHub
            </a>
          </Button>
          <Button className="hidden xs:inline-flex" asChild>
            <a href="https://api.research.computer" target="_blank">
              Try API
            </a>
          </Button>

          {/* Mobile Menu */}
          <div className="md:hidden">
            <NavigationSheet />
          </div>
        </div>
      </div>
    </nav>
  );
};

export default Navbar;
