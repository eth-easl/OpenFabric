import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { CirclePlay } from "lucide-react";
import AnimatedGlobe from "./animated-globe";
import { WalletConnect } from "./wallet-connect";

const Hero = () => {
  return (
    <div className="min-h-[calc(100vh-4rem)] w-full flex items-center justify-center overflow-hidden border-b border-accent">
      <div className="max-w-screen-xl w-full flex flex-col lg:flex-row mx-auto items-center justify-between gap-y-14 gap-x-10 px-6 py-12 lg:py-0">
        <div className="max-w-xl">
          <Badge className="rounded-full py-1 border-none">
            Read the Docs
          </Badge>
          <h1 className="mt-6 max-w-[20ch] text-3xl xs:text-4xl sm:text-5xl lg:text-[2.75rem] xl:text-5xl font-bold !leading-[1.2] tracking-tight">
            Compute Reimagined
          </h1>
          <p className="mt-6 max-w-[60ch] xs:text-lg">
            A peer-to-peer framework for decentralized computing. Connect computing resources globally, run ML inference at scale, and eliminate single points of failure with our LibP2P-based distributed system.
          </p>
          <div className="mt-12 flex flex-col sm:flex-row items-center gap-4">
            <WalletConnect />
            <Button
              variant="outline"
              size="lg"
              className="w-full sm:w-auto rounded-lg text-base shadow-none"
              onClick={() => window.open('https://docs.example.com', '_blank')}
            >
              <CirclePlay className="!h-5 !w-5" /> Try Demo
            </Button>
          </div>
        </div>
        <div className="relative lg:max-w-lg xl:max-w-xl w-full bg-gradient-to-br from-background via-accent/50 to-background rounded-xl aspect-square overflow-hidden">
          <AnimatedGlobe />
        </div>
      </div>
    </div>
  );
};

export default Hero;
